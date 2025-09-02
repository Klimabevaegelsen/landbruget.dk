-- Update Municipality Environmental Summary to include actual pesticide applications data
-- Migration: 20250901230000_update_environmental_summary_with_pesticides.sql

-- Drop existing view
DROP MATERIALIZED VIEW IF EXISTS municipality_environmental_summary;

-- Create updated Municipality Environmental Summary materialized view with pesticide data
CREATE MATERIALIZED VIEW municipality_environmental_summary AS
SELECT
    fb.municipality,
    2024 as year,  -- Fixed to 2024 for meaningful organic data

    -- Field-level environmental metrics
    COUNT(DISTINCT fb.id) as total_fields_with_env_data,
    AVG(fyd.n_leached_kg) as avg_n_leached_kg,
    SUM(fyd.n_leached_kg * fb.area_ha) / NULLIF(SUM(fb.area_ha), 0) as weighted_avg_n_leached,
    AVG(fyd.pesticide_load_index) as avg_pesticide_load_index,
    SUM(fyd.pesticide_load_index * fb.area_ha) / NULLIF(SUM(fb.area_ha), 0) as weighted_avg_pesticide_load,

    -- Environmental compliance metrics (assuming n_leached_kg is per hectare equivalent)
    COUNT(CASE WHEN fyd.n_leached_kg > 50 THEN 1 END) as fields_high_n_leaching,
    (COUNT(CASE WHEN fyd.n_leached_kg > 50 THEN 1 END)::float / NULLIF(COUNT(*), 0) * 100) as high_n_leaching_pct,

    COUNT(CASE WHEN fyd.pesticide_load_index > 1.0 THEN 1 END) as fields_high_pesticide_load,
    (COUNT(CASE WHEN fyd.pesticide_load_index > 1.0 THEN 1 END)::float / NULLIF(COUNT(*), 0) * 100) as high_pesticide_load_pct,

    -- ACTUAL PESTICIDE APPLICATIONS DATA (NEW)
    COALESCE(pest_stats.total_pesticide_applications, 0) as pesticide_applications_count,
    COALESCE(pest_stats.unique_pesticides_used, 0) as unique_pesticides_count,
    COALESCE(pest_stats.avg_health_burden, 0) as avg_pesticide_health_burden,
    COALESCE(pest_stats.avg_total_burden, 0) as avg_pesticide_total_burden,
    COALESCE(pest_stats.pfas_applications, 0) as pfas_applications_count,
    COALESCE(pest_stats.glyphosate_applications, 0) as glyphosate_applications_count,
    COALESCE(pest_stats.diquat_applications, 0) as diquat_applications_count,

    -- Organic farming as environmental indicator
    COUNT(CASE WHEN fyd.is_organic THEN 1 END) as organic_fields,
    (COUNT(CASE WHEN fyd.is_organic THEN 1 END)::float / NULLIF(COUNT(*), 0) * 100) as organic_percentage,
    SUM(CASE WHEN fyd.is_organic THEN fb.area_ha ELSE 0 END) as organic_area_ha,

    -- BNBO areas (environmental protection areas)
    COUNT(CASE WHEN fba.bnbo_status = 'not_dealt_with' THEN 1 END) as bnbo_not_dealt_with_count,
    SUM(CASE WHEN fba.bnbo_status = 'not_dealt_with' THEN fba.area_ha ELSE 0 END) as bnbo_not_dealt_with_area_ha,
    COUNT(CASE WHEN fba.bnbo_status IS NOT NULL THEN 1 END) as fields_with_bnbo_data,

    -- Wetland areas
    COUNT(CASE WHEN fwa.wetlands_status = 'not_restored' THEN 1 END) as wetland_not_restored_count,
    SUM(CASE WHEN fwa.wetlands_status = 'not_restored' THEN fwa.area_ha ELSE 0 END) as wetland_not_restored_area_ha,
    COUNT(CASE WHEN fwa.wetlands_status IS NOT NULL THEN 1 END) as fields_with_wetland_data,

    -- Enhanced environmental score calculation (lower is better) - now includes actual pesticide data
    (
        COALESCE(AVG(fyd.n_leached_kg), 0) * 0.25 +
        COALESCE(AVG(fyd.pesticide_load_index), 0) * 0.15 +
        COALESCE(pest_stats.avg_total_burden, 0) * 0.30 +  -- Real pesticide burden
        (100 - COALESCE((COUNT(CASE WHEN fyd.is_organic THEN 1 END)::float / NULLIF(COUNT(*), 0) * 100), 0)) * 0.30
    ) as environmental_burden_score

FROM field_boundaries fb
LEFT JOIN field_yearly_data fyd ON fb.field_uuid = fyd.field_uuid AND fyd.year = 2024
LEFT JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid AND fba.year = 2024
LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid AND fwa.year = 2024
-- NEW: Join with aggregated pesticide applications data
LEFT JOIN (
    SELECT
        municipality,
        COUNT(*) as total_pesticide_applications,
        COUNT(DISTINCT pesticide_name) as unique_pesticides_used,
        AVG(health_burden_score) as avg_health_burden,
        AVG(total_burden_score) as avg_total_burden,
        SUM(CASE WHEN contains_pfas THEN 1 ELSE 0 END) as pfas_applications,
        SUM(CASE WHEN contains_glyphosate THEN 1 ELSE 0 END) as glyphosate_applications,
        SUM(CASE WHEN contains_diquat THEN 1 ELSE 0 END) as diquat_applications
    FROM pesticide_applications
    WHERE year BETWEEN 2020 AND 2024  -- Last 5 years for recent environmental impact
    GROUP BY municipality
) pest_stats ON fb.municipality = pest_stats.municipality
WHERE fb.municipality IS NOT NULL
  AND fb.year = 2025  -- Use 2025 field boundaries
GROUP BY fb.municipality, pest_stats.total_pesticide_applications, pest_stats.unique_pesticides_used,
         pest_stats.avg_health_burden, pest_stats.avg_total_burden, pest_stats.pfas_applications,
         pest_stats.glyphosate_applications, pest_stats.diquat_applications;

-- Create indexes for performance
CREATE INDEX idx_municipality_environmental_summary_municipality ON municipality_environmental_summary(municipality);
CREATE INDEX idx_municipality_environmental_summary_year ON municipality_environmental_summary(year);
CREATE INDEX idx_municipality_environmental_summary_n_leached ON municipality_environmental_summary(avg_n_leached_kg DESC);
CREATE INDEX idx_municipality_environmental_summary_pesticide_load ON municipality_environmental_summary(avg_pesticide_load_index DESC);
CREATE INDEX idx_municipality_environmental_summary_organic_pct ON municipality_environmental_summary(organic_percentage DESC);
CREATE INDEX idx_municipality_environmental_summary_burden_score ON municipality_environmental_summary(environmental_burden_score ASC);
-- NEW indexes for pesticide data
CREATE INDEX idx_municipality_environmental_summary_pesticide_apps ON municipality_environmental_summary(pesticide_applications_count DESC);
CREATE INDEX idx_municipality_environmental_summary_health_burden ON municipality_environmental_summary(avg_pesticide_health_burden DESC);
CREATE INDEX idx_municipality_environmental_summary_total_burden ON municipality_environmental_summary(avg_pesticide_total_burden DESC);

-- Update the refresh function
CREATE OR REPLACE FUNCTION refresh_municipality_environmental_summary()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW municipality_environmental_summary;
END;
$$;

-- Grant appropriate permissions
GRANT SELECT ON municipality_environmental_summary TO anon;
GRANT SELECT ON municipality_environmental_summary TO authenticated;

-- Add comment
COMMENT ON MATERIALIZED VIEW municipality_environmental_summary IS
'Municipality-level aggregation of environmental metrics including nitrogen leaching, pesticide load, organic farming, BNBO areas, wetland restoration status, and ACTUAL pesticide applications data (2020-2024). Environmental burden score calculated where lower values indicate better environmental performance. Updated via refresh_municipality_environmental_summary() function.';
