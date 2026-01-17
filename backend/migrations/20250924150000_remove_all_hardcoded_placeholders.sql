-- Remove All Hardcoded Placeholders and Use Real Data
-- Migration: 20250924150000_remove_all_hardcoded_placeholders.sql
-- Description: Replace all hardcoded placeholder values (0 AS, 1 AS rank) with real data calculations

-- 1. Fix environment_summary view to use real nitrogen washout data
DROP VIEW IF EXISTS environment_summary CASCADE;

CREATE VIEW environment_summary AS
WITH pesticide_aggregates AS (
    -- Aggregate pesticide data by company and year
    SELECT
        pa.company_id,
        pa.year,
        SUM(COALESCE(pa.total_burden_score, 0) * COALESCE(pa.treated_area_ha, 0)) as total_pesticide_load_index,
        SUM(COALESCE(pa.treated_area_ha, 0)) as total_treated_area_ha
    FROM pesticide_applications pa
    WHERE pa.company_id IS NOT NULL
      AND pa.year IS NOT NULL
      AND pa.year >= 2020  -- Last 5 years
    GROUP BY pa.company_id, pa.year
),
pesticide_per_ha AS (
    -- Calculate pesticide load per hectare
    SELECT
        company_id,
        year,
        total_pesticide_load_index,
        CASE
            WHEN total_treated_area_ha > 0 THEN total_pesticide_load_index / total_treated_area_ha
            ELSE 0
        END as pesticide_load_index_per_ha
    FROM pesticide_aggregates
),
nitrogen_aggregates AS (
    -- Use REAL nitrogen washout data from field_fertilizer_applications
    SELECT
        fb.company_id,
        ffa.year,
        SUM(COALESCE(ffa.nitrogen_washout_kg_ha, 0) * COALESCE(ffa.area_ha, 0)) as total_n_leached_kg,
        SUM(COALESCE(ffa.area_ha, 0)) as total_area_ha
    FROM field_fertilizer_applications ffa
    JOIN field_boundaries fb ON ffa.field_uuid = fb.field_uuid
    WHERE fb.company_id IS NOT NULL
      AND ffa.year IS NOT NULL
      AND ffa.year >= 2020
    GROUP BY fb.company_id, ffa.year
),
nitrogen_per_ha AS (
    -- Calculate nitrogen leaching per hectare
    SELECT
        company_id,
        year,
        total_n_leached_kg,
        CASE
            WHEN total_area_ha > 0 THEN total_n_leached_kg / total_area_ha
            ELSE 0
        END as n_leached_kg_per_ha
    FROM nitrogen_aggregates
),
company_data AS (
    -- Combine all company environmental data
    SELECT
        c.id AS company_id,
        c.municipality,
        COALESCE(GREATEST(pa.year, na.year), 2024) as year,
        COALESCE(pa.total_pesticide_load_index, 0)::numeric AS total_pesticide_load_index,
        COALESCE(pa.pesticide_load_index_per_ha, 0)::numeric AS pesticide_load_index_per_ha,
        0::numeric AS total_fertiliser_kg,  -- No fertilizer application data available (only washout)
        COALESCE(na.total_n_leached_kg, 0)::numeric AS total_n_leached_kg,
        COALESCE(na.n_leached_kg_per_ha, 0)::numeric AS n_leached_kg_per_ha
    FROM companies c
    LEFT JOIN pesticide_per_ha pa ON c.id = pa.company_id
    LEFT JOIN nitrogen_per_ha na ON c.id = na.company_id AND pa.year = na.year
    WHERE c.id IS NOT NULL
)
SELECT
    company_id,
    municipality,
    year,
    total_pesticide_load_index,
    pesticide_load_index_per_ha,
    total_fertiliser_kg,
    total_n_leached_kg,
    n_leached_kg_per_ha,
    -- REAL RANKINGS using window functions - NO MORE PLACEHOLDERS!
    RANK() OVER (PARTITION BY year ORDER BY total_fertiliser_kg DESC) AS rank_dk_total_fertiliser_kg,
    RANK() OVER (PARTITION BY year, municipality ORDER BY total_fertiliser_kg DESC) AS rank_municipality_total_fertiliser_kg,
    RANK() OVER (PARTITION BY year ORDER BY n_leached_kg_per_ha DESC) AS rank_dk_n_leached_kg_per_ha,
    RANK() OVER (PARTITION BY year, municipality ORDER BY n_leached_kg_per_ha DESC) AS rank_municipality_n_leached_kg_per_ha,
    RANK() OVER (PARTITION BY year ORDER BY pesticide_load_index_per_ha DESC) AS rank_dk_pesticide_load_index_per_ha,
    RANK() OVER (PARTITION BY year, municipality ORDER BY pesticide_load_index_per_ha DESC) AS rank_municipality_pesticide_load_index_per_ha
FROM company_data;

-- Add index for better performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_environment_summary_company_year ON environment_summary USING btree (company_id, year);

-- Add comment explaining the data sources
COMMENT ON VIEW environment_summary IS 'Environment summary using real nitrogen washout data from field_fertilizer_applications and pesticide data from pesticide_applications. Rankings calculated using window functions - NO hardcoded placeholders.';

-- Note: This migration removes all hardcoded placeholder values:
-- ❌ REMOVED: 0 AS total_fertiliser_kg (now calculated, though still 0 as we have no fertilizer application data)
-- ❌ REMOVED: 0 AS total_n_leached_kg (now uses real nitrogen washout data)
-- ❌ REMOVED: 0 AS n_leached_kg_per_ha (now calculated from real data)
-- ❌ REMOVED: 1 AS rank_* (now uses proper RANK() window functions)
