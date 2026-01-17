-- Create meaningful municipality rankings based on real available data
-- These replace the nonsensical composite scores with specific, understandable metrics

-- Municipality Animal Production Summary (based on real production_sites data)
CREATE MATERIALIZED VIEW municipality_animal_production_summary AS
SELECT 
  municipality,
  COUNT(*) as total_production_sites,
  SUM(capacity) as total_animal_capacity,
  AVG(capacity) as avg_site_capacity,
  -- Count companies (via company_id)
  COUNT(DISTINCT company_id) as unique_companies,
  -- Current timestamp for tracking
  NOW() as last_updated
FROM production_sites
WHERE municipality IS NOT NULL 
  AND capacity IS NOT NULL 
  AND capacity > 0
GROUP BY municipality;

-- Create index for performance
CREATE INDEX idx_municipality_animal_production_summary_municipality 
ON municipality_animal_production_summary(municipality);

CREATE INDEX idx_municipality_animal_production_summary_total_capacity 
ON municipality_animal_production_summary(total_animal_capacity DESC);

-- Municipality Pesticide Summary (based on real pesticide application data)
CREATE MATERIALIZED VIEW municipality_pesticide_burden_summary AS
SELECT 
  paf.municipality,
  pa.year,
  -- Total pesticide burden
  SUM(pa.total_burden_score) as total_pesticide_burden,
  SUM(pa.health_burden_score) as total_health_burden,
  -- Specific pesticide types
  SUM(CASE WHEN pa.contains_glyphosate THEN pa.total_burden_score ELSE 0 END) as glyphosate_burden,
  SUM(CASE WHEN pa.contains_pfas THEN pa.total_burden_score ELSE 0 END) as pfas_burden,
  SUM(CASE WHEN pa.contains_diquat THEN pa.total_burden_score ELSE 0 END) as diquat_burden,
  -- Application counts
  COUNT(*) as total_applications,
  COUNT(CASE WHEN pa.contains_glyphosate THEN 1 END) as glyphosate_applications,
  COUNT(CASE WHEN pa.contains_pfas THEN 1 END) as pfas_applications,
  -- Treated area
  SUM(pa.treated_area_ha) as total_treated_area_ha,
  -- Companies
  COUNT(DISTINCT paf.cvr_number) as unique_companies,
  NOW() as last_updated
FROM pesticide_applications pa
JOIN pesticide_applications_with_field_details paf ON pa.field_uuid = paf.field_uuid
WHERE paf.municipality IS NOT NULL
  AND pa.year >= 2023  -- Include recent years
GROUP BY paf.municipality, pa.year;

-- Create indexes for performance
CREATE INDEX idx_municipality_pesticide_burden_summary_municipality_year 
ON municipality_pesticide_burden_summary(municipality, year);

CREATE INDEX idx_municipality_pesticide_burden_summary_total_burden 
ON municipality_pesticide_burden_summary(total_pesticide_burden DESC);

CREATE INDEX idx_municipality_pesticide_burden_summary_glyphosate_burden 
ON municipality_pesticide_burden_summary(glyphosate_burden DESC);

-- Refresh the materialized views
REFRESH MATERIALIZED VIEW municipality_animal_production_summary;
REFRESH MATERIALIZED VIEW municipality_pesticide_burden_summary;
