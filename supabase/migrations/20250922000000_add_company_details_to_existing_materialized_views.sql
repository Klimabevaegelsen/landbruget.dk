-- Add company details and agricultural filtering to existing materialized views
-- This eliminates the need for runtime joins in the rankings API

-- 1. Update worker_yearly_summary to include company details
DROP MATERIALIZED VIEW IF EXISTS worker_yearly_summary CASCADE;

CREATE MATERIALIZED VIEW worker_yearly_summary AS
WITH employee_yearly AS (
  -- Aggregate employee data by company and year (using latest month per year)
  SELECT
    company_id,
    EXTRACT(year FROM month_year)::integer as year,
    AVG(employee_count)::integer as average_employee_count
  FROM employee_monthly_counts
  WHERE employee_count > 0
  GROUP BY company_id, EXTRACT(year FROM month_year)
),
visa_yearly AS (
  -- Aggregate visa data by company and year (2024/2025 is the latest year with data)
  SELECT
    company_id,
    year,
    first_permits_count as active_visa_count
  FROM visa_yearly_counts
  WHERE first_permits_count > 0
  AND year <= 2025  -- Only include years with actual data
),
injury_yearly AS (
  -- Get injury data from incidents table (workplace inspections)
  SELECT
    company_id,
    EXTRACT(year FROM incident_date)::integer as year,
    COUNT(*)::text as injury_count_reported
  FROM incidents
  WHERE type = 'workplace_inspection'
  GROUP BY company_id, EXTRACT(year FROM incident_date)
),
all_companies_years AS (
  -- Get all company-year combinations from any of the data sources
  SELECT company_id, year FROM employee_yearly
  UNION
  SELECT company_id, year FROM visa_yearly
  UNION
  SELECT company_id, year FROM injury_yearly
)
SELECT
  gen_random_uuid() as id,
  acy.company_id,
  acy.year,
  -- Company details (pre-joined to eliminate runtime joins)
  c.cvr_number,
  c.company_name,
  c.municipality,
  c.is_agricultural_company,
  -- Worker metrics
  COALESCE(ey.average_employee_count, 0) as average_employee_count,
  COALESCE(vy.active_visa_count, 0) as active_visa_count,
  COALESCE(iy.injury_count_reported, '0') as injury_count_reported,
  CURRENT_TIMESTAMP as created_at,
  CURRENT_TIMESTAMP as updated_at
FROM all_companies_years acy
JOIN companies c ON acy.company_id = c.id  -- Join company details once during materialization
LEFT JOIN employee_yearly ey ON acy.company_id = ey.company_id AND acy.year = ey.year
LEFT JOIN visa_yearly vy ON acy.company_id = vy.company_id AND acy.year = vy.year
LEFT JOIN injury_yearly iy ON acy.company_id = iy.company_id AND acy.year = iy.year;

-- Create indexes for better performance
CREATE INDEX idx_worker_yearly_summary_company_year ON worker_yearly_summary(company_id, year);
CREATE INDEX idx_worker_yearly_summary_employees ON worker_yearly_summary(average_employee_count DESC) WHERE average_employee_count > 0;
CREATE INDEX idx_worker_yearly_summary_visas ON worker_yearly_summary(active_visa_count DESC) WHERE active_visa_count > 0;
CREATE INDEX idx_worker_yearly_summary_injuries ON worker_yearly_summary(injury_count_reported) WHERE injury_count_reported != '0';
-- Add agricultural company index for fast filtering
CREATE INDEX idx_worker_yearly_summary_agricultural ON worker_yearly_summary(is_agricultural_company, year) WHERE is_agricultural_company = true;

-- 2. Create a view that filters yearly_financials with company details for rankings
-- (yearly_financials is already a table, so we create a view for rankings)
DROP VIEW IF EXISTS yearly_financials_with_company_details CASCADE;

CREATE VIEW yearly_financials_with_company_details AS
SELECT
  yf.*,
  -- Company details (pre-joined)
  c.company_name,
  c.municipality,
  c.is_agricultural_company
FROM yearly_financials yf
JOIN companies c ON yf.company_id = c.id;

-- Create an index on the underlying yearly_financials table to support the view
CREATE INDEX IF NOT EXISTS idx_yearly_financials_company_id ON yearly_financials(company_id);

-- Grant permissions
GRANT SELECT ON worker_yearly_summary TO anon;
GRANT SELECT ON yearly_financials_with_company_details TO anon;

COMMENT ON MATERIALIZED VIEW worker_yearly_summary IS
'Worker data aggregated by company and year, with company details pre-joined to eliminate runtime joins in rankings API';

COMMENT ON VIEW yearly_financials_with_company_details IS
'Financial data with company details pre-joined for efficient rankings queries';
