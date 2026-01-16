-- Create RPC function for efficient pesticide company analysis
-- Migration: 20250103000002_create_pesticide_rpc_function.sql

CREATE OR REPLACE FUNCTION get_pesticide_companies(
  p_years INTEGER[] DEFAULT NULL,
  p_geography TEXT DEFAULT NULL,
  p_cvr BIGINT DEFAULT NULL,
  p_pesticide_type TEXT DEFAULT 'total',
  p_sort_by TEXT DEFAULT 'total_belastning',
  p_sort_order TEXT DEFAULT 'desc',
  p_limit INTEGER DEFAULT 50,
  p_offset INTEGER DEFAULT 0
)
RETURNS TABLE(
  cvr_number BIGINT,
  company_name TEXT,
  municipality TEXT,
  total_belastning NUMERIC,
  pfas_belastning NUMERIC,
  diquat_belastning NUMERIC,
  glyphosate_belastning NUMERIC,
  total_applications BIGINT,
  unique_products BIGINT,
  total_treated_area_ha NUMERIC,
  years_active INTEGER[]
) AS $$
DECLARE
  sort_column TEXT;
  order_direction TEXT;
BEGIN
  -- Map sort parameters to actual columns
  CASE p_sort_by
    WHEN 'applications' THEN sort_column := 'total_applications';
    WHEN 'area' THEN sort_column := 'total_treated_area_ha';
    WHEN 'belastning' THEN
      CASE p_pesticide_type
        WHEN 'pfas' THEN sort_column := 'pfas_belastning';
        WHEN 'diquat' THEN sort_column := 'diquat_belastning';
        WHEN 'glyphosate' THEN sort_column := 'glyphosate_belastning';
        ELSE sort_column := 'total_belastning';
      END CASE;
    ELSE sort_column := 'total_belastning';
  END CASE;

  order_direction := UPPER(p_sort_order);

  RETURN QUERY EXECUTE format('
    WITH filtered_data AS (
      SELECT
        cps.cvr_number,
        cps.company_name,
        COALESCE(cps.municipality, ''Unknown'') as municipality,
        cps.application_year,
        cps.total_belastning,
        cps.pfas_belastning,
        cps.diquat_belastning,
        cps.glyphosate_belastning,
        cps.total_applications,
        cps.unique_products,
        cps.total_treated_area_ha
      FROM company_pesticide_summary cps
      WHERE 1=1
        AND ($1 IS NULL OR cps.application_year = ANY($1))
        AND ($2 IS NULL OR cps.municipality = $2)
        AND ($3 IS NULL OR cps.cvr_number = $3)
    ),
    aggregated_companies AS (
      SELECT
        fd.cvr_number,
        MAX(fd.company_name) as company_name,
        MAX(fd.municipality) as municipality,
        SUM(fd.total_belastning) as total_belastning,
        SUM(fd.pfas_belastning) as pfas_belastning,
        SUM(fd.diquat_belastning) as diquat_belastning,
        SUM(fd.glyphosate_belastning) as glyphosate_belastning,
        SUM(fd.total_applications) as total_applications,
        MAX(fd.unique_products) as unique_products,
        -- Use most recent year''s area
        (array_agg(fd.total_treated_area_ha ORDER BY fd.application_year DESC))[1] as total_treated_area_ha,
        array_agg(DISTINCT fd.application_year ORDER BY fd.application_year) as years_active
      FROM filtered_data fd
      GROUP BY fd.cvr_number
    ),
    filtered_by_type AS (
      SELECT *
      FROM aggregated_companies
      WHERE CASE $4
        WHEN ''pfas'' THEN pfas_belastning > 0
        WHEN ''diquat'' THEN diquat_belastning > 0
        WHEN ''glyphosate'' THEN glyphosate_belastning > 0
        ELSE true
      END
    )
    SELECT
      fbt.cvr_number,
      fbt.company_name,
      fbt.municipality,
      fbt.total_belastning,
      fbt.pfas_belastning,
      fbt.diquat_belastning,
      fbt.glyphosate_belastning,
      fbt.total_applications,
      fbt.unique_products,
      fbt.total_treated_area_ha,
      fbt.years_active
    FROM filtered_by_type fbt
    ORDER BY %I %s
    LIMIT $5 OFFSET $6
  ', sort_column, order_direction)
  USING p_years, p_geography, p_cvr, p_pesticide_type, p_limit, p_offset;
END;
$$ LANGUAGE plpgsql;

-- Create function to get metadata (total counts, available years, etc.)
CREATE OR REPLACE FUNCTION get_pesticide_metadata(
  p_years INTEGER[] DEFAULT NULL,
  p_geography TEXT DEFAULT NULL,
  p_cvr BIGINT DEFAULT NULL
)
RETURNS TABLE(
  total_companies BIGINT,
  companies_with_pfas BIGINT,
  companies_with_diquat BIGINT,
  companies_with_glyphosate BIGINT,
  available_years INTEGER[],
  available_municipalities TEXT[]
) AS $$
BEGIN
  RETURN QUERY
  WITH filtered_data AS (
    SELECT
      cps.cvr_number,
      cps.municipality,
      cps.application_year,
      cps.pfas_belastning,
      cps.diquat_belastning,
      cps.glyphosate_belastning
    FROM company_pesticide_summary cps
    WHERE 1=1
      AND (p_years IS NULL OR cps.application_year = ANY(p_years))
      AND (p_geography IS NULL OR cps.municipality = p_geography)
      AND (p_cvr IS NULL OR cps.cvr_number = p_cvr)
  ),
  company_aggregates AS (
    SELECT
      fd.cvr_number,
      MAX(fd.municipality) as municipality,
      SUM(fd.pfas_belastning) as total_pfas,
      SUM(fd.diquat_belastning) as total_diquat,
      SUM(fd.glyphosate_belastning) as total_glyphosate
    FROM filtered_data fd
    GROUP BY fd.cvr_number
  )
  SELECT
    COUNT(DISTINCT ca.cvr_number) as total_companies,
    COUNT(DISTINCT CASE WHEN ca.total_pfas > 0 THEN ca.cvr_number END) as companies_with_pfas,
    COUNT(DISTINCT CASE WHEN ca.total_diquat > 0 THEN ca.cvr_number END) as companies_with_diquat,
    COUNT(DISTINCT CASE WHEN ca.total_glyphosate > 0 THEN ca.cvr_number END) as companies_with_glyphosate,
    (SELECT array_agg(DISTINCT application_year ORDER BY application_year)
     FROM company_pesticide_summary
     WHERE application_year IS NOT NULL) as available_years,
    (SELECT array_agg(DISTINCT municipality ORDER BY municipality)
     FROM company_pesticide_summary
     WHERE municipality IS NOT NULL AND municipality != 'Unknown') as available_municipalities
  FROM company_aggregates ca;
END;
$$ LANGUAGE plpgsql;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION get_pesticide_companies TO authenticated, anon;
GRANT EXECUTE ON FUNCTION get_pesticide_metadata TO authenticated, anon;
